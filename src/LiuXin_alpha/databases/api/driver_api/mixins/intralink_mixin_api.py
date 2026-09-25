"""
Specify SQL generation for relationships within one main table.

Generated SQL can include type restrictions, endpoint indexes and symmetry guards.
Generation does not execute the statements. Shared SQLite symmetry guards reject
reversed endpoints rather than swapping them.
"""

from __future__ import annotations

import abc
from typing import Optional, Iterable, Union


class DriverIntralinkMixinAPI(abc.ABC):
    """
    Specify SQL generation for relationships within one main table.

    Generated SQL can include type restrictions, endpoint indexes and symmetry guards.
    Generation does not execute the statements. Shared SQLite symmetry guards reject
    reversed endpoints rather than swapping them. Abstract members must be implemented
    by a backend; their empty bodies return None when called directly.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DriverIntralinkMixinAPI)
        True
    """

    @abc.abstractmethod
    def direct_build_allowed_types_table_intralink(
            self,
            for_table: str,
            allowed_types: Optional[Iterable[str]] = None
    ) -> list[str]:
        """
        Build a legacy intralink type table using explicit labels or stored defaults.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Assert membership when the host exposes intralink_tables. Falsy resolved labels
        produce no statements; interpolated seed labels are not escaped.

        Example:
            >>> driver.direct_build_allowed_types_table_intralink("works", ["related"])  # doctest: +SKIP


        :param for_table: Table whose allowed-type lookup name is derived.
        :param allowed_types: Optional iterable of permitted type labels.
        :return: DDL/seed statements or an empty list.
        """

    # Todo: We're gonna need to rename SQL to SQLite - where appropriate
    @abc.abstractmethod
    def direct_build_intralink_table_sql(
            self,
            name: str,
            allowed_types: Optional[Iterable[str]] = None,
            requested_cols: Optional[Union[str, Iterable[str], set[str]]] = None,
            index_both: bool = True,
            nullable_fks: bool = True,
            symmetric: bool = False,
            symmetric_types: Optional[Iterable[str]] = None,
            use_reference_types_table: bool = False) -> list[str]:
        """
        Generate self-link DDL, indexes and optional canonical-order guard triggers.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        None selects type; allowed labels force a type column. Always include source and
        reject nonnull self-edges. Symmetric guards reject reversed endpoints rather than
        swapping them; symmetric_types restricts that guard even when symmetric is true.
        Reference-type mode omits the FK and requires later guard installation. Explicit
        sequence_number/is_required subsets currently emit those columns twice because the
        bespoke-column filter also includes them; all avoids that duplication.

        Example:
            >>> driver.direct_build_intralink_table_sql("works")  # doctest: +SKIP


        :param name: Main-table name, optionally resolved by the host match_to_table_name
            hook.
        :param allowed_types: Optional iterable of permitted type labels.
        :param requested_cols: all, None, or iterable of optional metadata suffixes.
        :param index_both: Whether to index both endpoint foreign-key columns.
        :param nullable_fks: Whether the two self-link foreign keys are nullable.
        :param symmetric: Whether to guard canonical ordering for all types when no subset
            is supplied.
        :param symmetric_types: Optional reusable type-label collection limiting
            canonical-order guards.
        :param use_reference_types_table: Whether a separately installed __types guard
            replaces legacy allowed-type FKs.
        :return: Ordered statements; no SQL is executed.
        """
