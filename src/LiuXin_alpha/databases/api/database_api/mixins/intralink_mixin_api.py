"""
Directed relationships within one database table.

Abstract declarations describe the concrete facade conventions; their bodies do not execute database operations. Duplicate method declarations are retained; later definitions determine the runtime class. Concrete get_intralinked_rows and secondary-only deletion have documented legacy limitations.
"""

from __future__ import annotations

import abc
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import RowAPI


class DatabaseIntralinkRowsMixinAPI(abc.ABC):
    """
    Declare directed relationships within one database table.

    Implement all abstract members before instantiation. Duplicate method declarations are retained; later definitions determine the runtime class. Concrete get_intralinked_rows and secondary-only deletion have documented legacy limitations.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DatabaseIntralinkRowsMixinAPI)
        True
    """

    @abc.abstractmethod
    def intralink_rows(self, primary_row: "RowAPI", secondary_row: "RowAPI", link_type: str) -> "RowAPI":
        """
        Create and synchronize a typed directed link between rows in the same table.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Validate equal table names and IDs, then consult allowed_<table>_intralink_types preferences if present. Resolve endpoint/type columns, allocate an ID and sync. Later schema constraints still apply; failed synchronization has no explicit cleanup of an allocated placeholder.

        Example:
            With related permitted for the table, link = db.intralink_rows(first, second, " RELATED ") stores the normalized type related.


        :param primary_row: Primary endpoint with a non-None row ID.
        :param secondary_row: Secondary endpoint in the same table, also with an ID.
        :param link_type: Type converted to text, stripped and lowercased before validation/storage.
        :return: Persisted generic Row for the self-link.
        :raises InputIntegrityError: Tables differ, an endpoint ID is missing, or preferences reject the normalized type.
        """

    @abc.abstractmethod
    def get_intralink_row(self, primary_row: "RowAPI", secondary_row: "RowAPI") -> Optional["RowAPI"]:
        """
        Find the unique directed self-link for an endpoint pair.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Validate that a self-link table exists, search by primary ID, then compare secondary IDs as text. Type is not filtered, so differently typed matches can be ambiguous.

        Example:
            For same-table Rows, db.get_intralink_row(first, second) looks only in the first-to-second direction.


        :param primary_row: Primary endpoint.
        :param secondary_row: Secondary endpoint from the same table.
        :return: Matching link Row, or None when absent.
        :raises InputIntegrityError: Tables differ or the self-link table is unavailable.
        :raises DatabaseIntegrityError: More than one link matches the directed pair.
        """

    # Todo: Merge with the method below
    @abc.abstractmethod
    def get_intralink_rows(
        self,
        row: "RowAPI",
        primary: bool = True,
        secondary: bool = True,
        link_type_filter: Optional[str] = None,
    ) -> list["RowAPI"]:
        """
        Collect self-link Rows mentioning a seed in either endpoint column.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: There is no deduplication or priority sort; a self-link can appear twice when both directions are requested. With both direction flags false the result is empty, although schema columns are still resolved.

        Example:
            For a seed Row, db.get_intralink_rows(seed, primary=False, secondary=True) returns incoming self-links.


        :param row: Seed Row.
        :param primary: Include relationships where the seed is primary.
        :param secondary: Include relationships where the seed is secondary.
        :param link_type_filter: Optional type compared as text without stripping or lowercasing.
        :return: Link Row list, primary-query results followed by secondary-query results.
        """

    @abc.abstractmethod
    def get_intralinked_rows(
        self,
        primary_row: Optional["RowAPI"],
        secondary_row: Optional["RowAPI"],
    ) -> list["RowAPI"]:
        """
        Search one self-link direction while retaining the legacy link-row return value.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Exactly one seed must be non-None. The method still loads opposite endpoint Rows before returning link Rows, so endpoint-loading errors can propagate without contributing returned values.

        Example:
            links = db.get_intralinked_rows(primary_row=seed, secondary_row=None) currently returns outgoing relationship records, not their target records.


        :param primary_row: Outgoing seed, or None when using secondary_row.
        :param secondary_row: Incoming seed, or None when using primary_row.
        :return: Matching link Rows; despite the method name, the separately loaded endpoint Rows are discarded.
        :raises InputIntegrityError: Both seeds are supplied or both are None.
        """

    @abc.abstractmethod
    def unlinked_intralink(self, primary_row: Optional["RowAPI"], secondary_row: Optional["RowAPI"]) -> None:
        """
        Delete a unique directed pair or every outgoing link from a primary seed.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Both seeds select a unique pair; only a primary seed selects all outgoing links. The secondary-only branch incorrectly reads primary_row.table while primary_row is None and raises AttributeError. There is no grouped rollback for multi-row deletion.

        Example:
            For a valid seed, db.unlinked_intralink(primary_row=seed, secondary_row=None) removes its outgoing links and retains endpoint records.


        :param primary_row: Primary seed, or None for the legacy incoming-only branch.
        :param secondary_row: Secondary seed, or None to delete all outgoing links.
        :return: None; a missing explicitly requested pair is a no-op.
        :raises InputIntegrityError: Both seeds are None.
        :raises AttributeError: Only secondary_row is supplied, due to the legacy branch defect.
        :raises DatabaseIntegrityError: A requested pair has more than one link.
        """

    # Todo: unlink_all_intralinks

    # ---------------------------------------------------------------------------------------------
    # Intralink tables (many-to-many within the *same* table)
    # ---------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def intralink_rows(self, primary_row: "RowAPI", secondary_row: "RowAPI", link_type: str) -> "RowAPI":
        """
        Create and synchronize a typed directed link between rows in the same table.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Validate equal table names and IDs, then consult allowed_<table>_intralink_types preferences if present. Resolve endpoint/type columns, allocate an ID and sync. Later schema constraints still apply; failed synchronization has no explicit cleanup of an allocated placeholder.

        Example:
            With related permitted for the table, link = db.intralink_rows(first, second, " RELATED ") stores the normalized type related.


        :param primary_row: Primary endpoint with a non-None row ID.
        :param secondary_row: Secondary endpoint in the same table, also with an ID.
        :param link_type: Type converted to text, stripped and lowercased before validation/storage.
        :return: Persisted generic Row for the self-link.
        :raises InputIntegrityError: Tables differ, an endpoint ID is missing, or preferences reject the normalized type.
        """

    @abc.abstractmethod
    def get_intralink_row(self, primary_row: "RowAPI", secondary_row: "RowAPI") -> Optional["RowAPI"]:
        """
        Find the unique directed self-link for an endpoint pair.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Validate that a self-link table exists, search by primary ID, then compare secondary IDs as text. Type is not filtered, so differently typed matches can be ambiguous.

        Example:
            For same-table Rows, db.get_intralink_row(first, second) looks only in the first-to-second direction.


        :param primary_row: Primary endpoint.
        :param secondary_row: Secondary endpoint from the same table.
        :return: Matching link Row, or None when absent.
        :raises InputIntegrityError: Tables differ or the self-link table is unavailable.
        :raises DatabaseIntegrityError: More than one link matches the directed pair.
        """

    @abc.abstractmethod
    def get_intralink_rows(
        self,
        row: "RowAPI",
        primary: bool = True,
        secondary: bool = True,
        link_type_filter: Optional[str] = None,
    ) -> list["RowAPI"]:
        """
        Collect self-link Rows mentioning a seed in either endpoint column.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: There is no deduplication or priority sort; a self-link can appear twice when both directions are requested. With both direction flags false the result is empty, although schema columns are still resolved.

        Example:
            For a seed Row, db.get_intralink_rows(seed, primary=False, secondary=True) returns incoming self-links.


        :param row: Seed Row.
        :param primary: Include relationships where the seed is primary.
        :param secondary: Include relationships where the seed is secondary.
        :param link_type_filter: Optional type compared as text without stripping or lowercasing.
        :return: Link Row list, primary-query results followed by secondary-query results.
        """

    @abc.abstractmethod
    def get_intralinked_rows(
        self,
        primary_row: Optional["RowAPI"],
        secondary_row: Optional["RowAPI"],
    ) -> list["RowAPI"]:
        """
        Search one self-link direction while retaining the legacy link-row return value.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Exactly one seed must be non-None. The method still loads opposite endpoint Rows before returning link Rows, so endpoint-loading errors can propagate without contributing returned values.

        Example:
            links = db.get_intralinked_rows(primary_row=seed, secondary_row=None) currently returns outgoing relationship records, not their target records.


        :param primary_row: Outgoing seed, or None when using secondary_row.
        :param secondary_row: Incoming seed, or None when using primary_row.
        :return: Matching link Rows; despite the method name, the separately loaded endpoint Rows are discarded.
        :raises InputIntegrityError: Both seeds are supplied or both are None.
        """

    @abc.abstractmethod
    def unlinked_intralink(self, primary_row: Optional["RowAPI"], secondary_row: Optional["RowAPI"]) -> None:
        """
        Delete a unique directed pair or every outgoing link from a primary seed.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Both seeds select a unique pair; only a primary seed selects all outgoing links. The secondary-only branch incorrectly reads primary_row.table while primary_row is None and raises AttributeError. There is no grouped rollback for multi-row deletion.

        Example:
            For a valid seed, db.unlinked_intralink(primary_row=seed, secondary_row=None) removes its outgoing links and retains endpoint records.


        :param primary_row: Primary seed, or None for the legacy incoming-only branch.
        :param secondary_row: Secondary seed, or None to delete all outgoing links.
        :return: None; a missing explicitly requested pair is a no-op.
        :raises InputIntegrityError: Both seeds are None.
        :raises AttributeError: Only secondary_row is supplied, due to the legacy branch defect.
        :raises DatabaseIntegrityError: A requested pair has more than one link.
        """
