
"""
Specify insertion of individual rows and homogeneous row batches.

Implementations resolve tables from row keys and own transaction handling. The
shared SQL writers copy input mappings and commit a batch only on success;
failures roll back the complete operation.
"""

import abc

from typing import Iterable, Any


class DriverAddMixinAPI(abc.ABC):
    """
    Specify insertion of individual rows and homogeneous row batches.

    Implementations resolve tables from row keys and own transaction handling. The
    shared SQL writers copy input mappings and commit a batch only on success;
    failures roll back the complete operation. Abstract members must be implemented by
    a backend; their empty bodies return None when called directly.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DriverAddMixinAPI)
        True
    """

    @abc.abstractmethod
    def direct_add_multiple_simple_row_dicts(self, row_dict_list: Iterable[dict[str, Any]]) -> None:
        """
        Insert a batch atomically, binding values by named column.

        The shared SQL/SQLite writer requires one table, matching column sets and no
        non-null explicit IDs. Input dictionaries may have different key orders and are
        not mutated. Normalization and custom-column NUL handling match single inserts.
        A failure rolls back the whole batch and closes its owned connection. PostgreSQL
        also rolls back the full batch while retaining its primary connection.

        Example:
            A uniqueness violation on the second row leaves neither row inserted.

        :param row_dict_list: Iterable of row mappings; shared SQL requires homogeneous
            rows.
        :return: True for an empty shared-SQL batch; otherwise None. PostgreSQL returns
            True.
        """

    @abc.abstractmethod
    def direct_add_simple_row_dict(self, row_dict: dict[str, Any]) -> int:
        """
        Insert a copied row mapping, deriving configured normalized identity values.

        The shared SQL/SQLite writer sanitizes NUL text in custom-column values, commits
        only on success and always closes its connection. PostgreSQL retains its primary
        connection. Backend exceptions propagate through this interface.

        Example:
            A work_title value of "None" remains ordinary text after insertion.

        :param row_dict: Column/value mapping, optionally including a table hint; not
            mutated.
        :return: The inserted row ID.
        """
