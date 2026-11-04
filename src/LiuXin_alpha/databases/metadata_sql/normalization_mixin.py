"""Audit and repair declared physical comparison keys without changing schema."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from LiuXin_alpha.databases.db_types.macro_types import (
    NormalizedIdentityMigrationReport,
)

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class NormalizationSQLMixin:
    """Expose scoped, data-only maintenance using the shared declaration owner.

    Example:
        ``db.metadata_sql.audit_normalized_keys("tags", row_ids=[tag_id])``
        checks a selected Tag's key against the table's unchanged stored keys.
    """

    db: DatabaseAPI

    def audit_normalized_keys(
        self,
        table: str,
        *,
        value_columns: Iterable[str] | None = None,
        row_ids: Iterable[int] | None = None,
    ) -> NormalizedIdentityMigrationReport:
        """Report selected stale keys and collisions without changing data/schema.

        Select by declared display-column names and physical row IDs. Collision
        detection scans the table, comparing planned keys with unchanged stored
        keys on other rows. A column suffix alone does not declare an identity.
        """
        return self.db.macros.audit_normalized_keys(
            table, value_columns=value_columns, row_ids=row_ids
        )

    def rebuild_normalized_keys(
        self,
        table: str,
        *,
        value_columns: Iterable[str] | None = None,
        row_ids: Iterable[int] | None = None,
    ) -> NormalizedIdentityMigrationReport:
        """Repair selected keys atomically, preserving display values and schema.

        Recheck collisions in the write transaction before effects. Schema
        constraints still apply. Empty selections are no-ops; None selects all
        declared identities/rows. An enclosing transaction retains ownership.
        """
        return self.db.macros.rebuild_normalized_keys(
            table, value_columns=value_columns, row_ids=row_ids
        )
