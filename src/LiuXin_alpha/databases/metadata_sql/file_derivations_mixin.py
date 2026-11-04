"""Batch reads of physical file_derivations rows; policy stays with callers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class FileDerivationsSQLMixin:
    """Own queries returning file_derivations rows on the current connection.

    Example:
        Access these operations through ``db.metadata_sql``.
    """

    db: DatabaseAPI

    def get_file_derivations_for_parents(
        self,
        file_ids: Iterable[int],
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Batch rows by file_derivation_parent_file_id, retaining empty owner groups.

        Order each group by physical row ID. Do not require owners to exist or
        interpret stored metadata. Reads share the caller's current transaction.
        """
        return self.db.macros.get_rows_for_ids(
            "file_derivations",
            "file_derivation_parent_file_id",
            file_ids,
        )

    def get_file_derivations_for_children(
        self,
        file_ids: Iterable[int],
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Batch rows by file_derivation_child_file_id, retaining empty owner groups.

        Order each group by physical row ID. Do not require owners to exist or
        interpret stored metadata. Reads share the caller's current transaction.
        """
        return self.db.macros.get_rows_for_ids(
            "file_derivations",
            "file_derivation_child_file_id",
            file_ids,
        )
