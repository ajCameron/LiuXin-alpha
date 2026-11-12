"""
Directed relationships within one database table.

Abstract declarations describe the concrete facade conventions; their bodies do not
execute database operations. get_intralinked_rows returns opposite endpoint Rows.
"""

from __future__ import annotations

import abc
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import RowAPI


class DatabaseIntralinkRowsMixinAPI(abc.ABC):
    """
    Declare directed relationships within one database table.

    Implement all abstract members before instantiation. get_intralinked_rows
    returns opposite endpoint Rows.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DatabaseIntralinkRowsMixinAPI)
        True
    """

    # Todo: Merge with the method below

    # Todo: unlink_all_intralinks

    # ---------------------------------------------------------------------------------------------
    # Intralink tables (many-to-many within the *same* table)
    # ---------------------------------------------------------------------------------------------
    @abc.abstractmethod
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

    @abc.abstractmethod
    def get_intralink_row(
        self, primary_row: "RowAPI", secondary_row: "RowAPI"
    ) -> Optional["RowAPI"]:
        """
        Find the unique directed self-link for an endpoint pair.

        Abstract hook; subclasses supply the operation. Concrete Database behavior:
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

        Abstract hook; subclasses supply the operation. Concrete Database behavior:
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

    @abc.abstractmethod
    def get_intralinked_rows(
        self,
        primary_row: Optional["RowAPI"],
        secondary_row: Optional["RowAPI"],
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

    @abc.abstractmethod
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
