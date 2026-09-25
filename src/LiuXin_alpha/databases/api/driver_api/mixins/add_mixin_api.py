
"""
Specify insertion of individual rows and homogeneous row batches.

Implementations resolve tables from row keys and own transaction handling. The
shared SQL writers can mutate supplied mappings and commit partial work on failure;
callers must not infer batch atomicity from these signatures.
"""

import abc

from typing import Iterable, Any


class DriverAddMixinAPI(abc.ABC):
    """
    Specify insertion of individual rows and homogeneous row batches.

    Implementations resolve tables from row keys and own transaction handling. The
    shared SQL writers can mutate supplied mappings and commit partial work on failure;
    callers must not infer batch atomicity from these signatures. Abstract members must
    be implemented by a backend; their empty bodies return None when called directly.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DriverAddMixinAPI)
        True
    """

    @abc.abstractmethod
    def direct_add_multiple_simple_row_dicts(self, row_dict_list: Iterable[dict[str, Any]]) -> None:
        """
        Insert rows sharing a table, the same keys and the same key order.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Mismatched tables/key sets or non-null explicit IDs raise InputIntegrityError.
        Values follow each mapping's insertion order, so matching key sets alone are
        insufficient. Identity derivation and NUL sanitization follow single-row insertion.
        Commit/close run in finally, allowing partial batches to persist on errors.

        Example:
            >>> driver.direct_add_multiple_simple_row_dicts([{"work_title": "Example"}])  # doctest: +SKIP


        :param row_dict_list: Sized, indexable sequence of homogeneous row mappings;
            inference and sanitization may mutate them.
        :return: ``True`` for an empty batch; otherwise ``None``.
        """

    @abc.abstractmethod
    def direct_add_simple_row_dict(self, row_dict: dict[str, Any]) -> int:
        """
        Infer a table, derive configured identity values and insert one bound-value row.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Custom-column value tables also sanitize NUL text. The connection commits and closes
        in finally, even on execution errors; this is not a rollback-on-error helper. SQLite
        operational/integrity errors become driver/integrity errors.

        Example:
            >>> driver.direct_add_simple_row_dict({"work_id": 1})  # doctest: +SKIP


        :param row_dict: Column-to-value mapping; table inference removes any ``table`` key
            in place.
        :return: The cursor lastrowid for the inserted row.
        """
