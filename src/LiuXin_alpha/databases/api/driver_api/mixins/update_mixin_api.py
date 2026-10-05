
"""
Specify bulk value updates and individual row-dictionary writes.

Shared SQL scalar updates require a field and infer its table; individual row writes require an ID. Both paths handle their own commits. These stubs do not implement transactions or promise
batch atomicity.
"""

import abc

from typing import Iterable, Any


class DriverUpdateMixinAPI(abc.ABC):
    """
    Specify bulk value updates and individual row-dictionary writes.

    Shared SQL scalar updates require a field and infer its table; individual row writes require an ID. Both paths handle their own commits. These stubs do not implement transactions or promise
    batch atomicity. Abstract members must be implemented by a backend; their empty
    bodies return None when called directly.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DriverUpdateMixinAPI)
        True
    """
    @abc.abstractmethod
    def direct_update_columns(
            self,
            id_values_map: dict[str, Any],
            field: str = None,
            table: str = None) -> None:
        """
        Update one field for each ID, also deriving its configured identity column when available.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Infer the table from the field; a conflicting table argument only warns and the
        inferred table wins. Commit the batch on success and close in finally. Empty
        mappings are no-ops; dict-valued mappings select an unimplemented multi-column mode.

        Example:
            >>> driver.direct_update_columns({1: "Revised"}, field="work_title")  # doctest: +SKIP


        :param id_values_map: Mapping from row IDs to scalar field values; dict values are
            currently unsupported.
        :param field: Trusted column heading required for scalar mode.
        :param table: Optional expected table name; a mismatch warns rather than rejecting
            the update.
        :return: ``None``.
        """

    @abc.abstractmethod
    def direct_update_row_dict(self, row_dict: dict[str, Any]) -> None:
        """
        Update the identified row using its ID and the remaining supplied columns.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Infer the table before copying the mapping, convert exact text ``None`` values to
        null and derive configured identity fields. Missing ID raises RowIntegrityError. Use
        a plain dict, since a live Row object can recurse through its writer. Commit/close
        on success; handled SQLite errors are translated, with an additional commit on the
        integrity-error path.

        Example:
            >>> driver.direct_update_row_dict({"work_id": 1})  # doctest: +SKIP


        :param row_dict: Plain column/value dictionary including the ID; inference may
            remove its ``table`` key before copying.
        :return: ``True`` for an ID-only no-op; otherwise ``None`` after executing the
            update.
        """


