
"""
Mixin for adding link row manipulation ability to the database.
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
    Helper methods for pulling rows linked to a seed row.

    This deliberately keeps no cache. It is a small convenience layer over the existing
    interlink/intralink search primitives so higher layers can decide for themselves whether
    memoization is worthwhile.
    """

    def _coerce_link_seed_row(self: "DatabaseAPI", seed_row: "RowAPI | dict[str, Any]") -> "RowAPI":
        """
        Normalize a seed row into a live :class:`Row` tied to this database.

        :param seed_row:
        :return:
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
        Validate a target table for linked-row helpers.

        :param target_table:
        :return:
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
        """Return rows in ``target_table`` linked to ``seed_row``.

        This is intentionally a thin convenience layer. For the seed row's own table, the
        returned list contains just the seed row. For other tables, this delegates to the
        existing interlink search path, which already returns priority-ordered rows where
        priority exists.
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
        Return the first linked row in ``target_table`` or ``None``.

        :param seed_row:
        :param target_table:
        :param type_filter:
        :return:
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
        Return the ids for rows in ``target_table`` linked to ``seed_row``.

        :param seed_row:
        :param target_table:
        :param type_filter:
        :return:
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
        Return a fingerprint of rows linked to ``seed_row``.

        The output format is ``{"table_id"}``, e.g. ``{"creators_12", "series_8"}``.

        :param seed_row:
        :param target_tables:
        :param type_filter:
        :return:
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
